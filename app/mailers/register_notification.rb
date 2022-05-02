class RegisterNotification < ApplicationMailer
  def notify(user, share)
    @share = share 

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your account has been registered."
    )
  end
end
